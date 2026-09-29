from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("manager_id").orderBy("salary")

remaining_keys = (
    df_employees
    .select("level", "job_title")
    .subtract(
        df_employees
        .filter(F.col("manager_id") == 13)
        .select("level", "job_title")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id").eqNullSafe(F.col("p2.id")), "left")
    .select(
        "e.full_name",
        "e.manager_id",
        "e.salary",
        "e.is_remote",
        F.col("p2.full_name").alias("employee_full_name"),
    )
)

metrics = (
    enriched
    .filter(~F.col("full_name").like("Dav%"))
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.dense_rank().over(w2).alias("salary_dense_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share_2"),
        F.dense_rank().over(w3).alias("salary_dense_rank_2"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.full_name",
        "a.manager_id",
        "a.is_remote",
        "a.salary_share_2",
        "a.salary_dense_rank_2",
        F.col("b.full_name").alias("employee_full_name_2"),
    )
)

filtered = (
    with_dims
    .filter(F.col("salary_share_2") >= 0.35)
    .select(
        "manager_id",
        "is_remote",
        "salary_dense_rank_2",
        "employee_full_name_2",
        F.round((F.col("salary_share_2") * 1.07), 2).alias("salary_share_2_scaled"),
        F.coalesce("salary_share_2", F.lit(0)).alias("salary_share_2_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("salary_share_2_scaled") >= 87, "HIGH").otherwise("LOW").alias("salary_share_2_scaled_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.percentile_approx("salary_share_2_scaled", [0.25, 0.5, 0.75]).alias("salary_share_2_scaled_quartiles"),
        F.percentile_approx("salary_share_2_filled", [0.25, 0.5, 0.75]).alias("salary_share_2_filled_quartiles"),
        F.percentile_approx("salary_dense_rank_2", 0.5).alias("salary_dense_rank_2_p50"),
        F.countDistinct("manager_id").alias("distinct_manager_ids_2"),
    )
    .filter(F.col("salary_dense_rank_2_p50") >= 0)
    .orderBy(F.col("distinct_manager_ids").asc_nulls_last())
)
