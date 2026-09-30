from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy(F.month("hire_date"), "manager_id").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("manager_id")
w4 = Window.partitionBy("manager_id").orderBy("salary", "id")
w5 = Window.partitionBy("level").orderBy("salary_share", "id")
w6 = Window.partitionBy("level")
w7 = Window.orderBy(F.col("salary_share").asc_nulls_first())
w8 = Window.partitionBy("level").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("level").orderBy(F.col("salary_share_cume_dist").desc_nulls_first(), "id")
w10 = Window.partitionBy("level").orderBy("level_min_salary_share", "id")

filtered = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("job_title").eqNullSafe(F.lit("Analyst"))))
    .select(
        "*",
        F.concat("job_title", F.lit("/"), "email").alias("job_title_email"),
        F.coalesce("level", F.lit("OTHER")).alias("level_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("salary") <= 106000)
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        "job_title_email",
        "level_filled",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "email",
        "is_remote",
        "level",
        "job_title_email",
        "level_filled",
        "salary_share",
    )
)

metrics = (
    ranked
    .select(
        "id",
        "is_remote",
        "level",
        "level_filled",
        F.ntile(5).over(w5).alias("salary_share_bucket"),
        F.min("salary_share").over(w6).alias("level_min_salary_share"),
        F.cume_dist().over(w7).alias("salary_share_cume_dist"),
        F.sum("salary_share").over(w8).alias("cumulative_salary_share"),
    )
)

ranked_employees = (
    metrics
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked_employees
    .withColumn("row_num_2", F.row_number().over(w10))
    .filter(F.col("row_num_2") == 1)
    .select("id", "is_remote", "level")
)

remaining_keys = (
    deduped
    .select("level", "id")
    .intersectAll(
        df_employees
        .filter(F.col("dept_id").isin([7, 15, 16, 26]))
        .select("level", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("level")
    .agg(
        F.count_if(F.col("level") == "L2").alias("l2_events"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_rows"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("num_levels").desc())
)
