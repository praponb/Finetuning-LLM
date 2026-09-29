from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name", "job_title").orderBy(F.col("salary").desc())
w_hist = Window.partitionBy("dept_name").orderBy(F.col("salary").desc_nulls_first())
w4 = Window.orderBy(F.col("salary").asc_nulls_first())
w5 = Window.orderBy("salary", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("job_title").orderBy("salary_rank", "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("clean_location", F.trim("location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("NONE")))
)

with_latest = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        "s.clean_location",
        F.col("c.salary"),
        F.col("c.job_title"),
    )
)

valid = (
    with_latest
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Marketing")))
    .withColumn("job_title_prefix", F.substring("job_title", 1, 3))
    .withColumn("clean_location_2", F.trim("location"))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

with_windows = (
    valid
    .select(
        "id",
        "budget",
        "clean_location",
        "salary",
        "job_title",
        "job_title_prefix",
        "clean_location_2",
        F.rank().over(w2).alias("salary_rank"),
        F.dense_rank().over(w_hist).alias("salary_dense_rank"),
        F.rank().over(w4).alias("salary_rank_2"),
        F.avg("salary").over(w5).alias("salary_avg_6_prior"),
    )
)

with_latest_departments = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"))
)

deduped = (
    with_latest_departments
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "clean_location_2", "salary_rank", "salary_avg_6_prior")
)

matched_keys = (
    deduped
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("location") == "Tokyo")
        .select("id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup(
        F.when(F.col("salary_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("salary_rank_tier"),
    )
    .agg(
        F.percentile_approx("salary_avg_6_prior", 0.5).alias("salary_avg_6_prior_p50"),
        F.percentile_approx("salary_avg_6_prior", [0.25, 0.5, 0.75]).alias("salary_avg_6_prior_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("salary_avg_6_prior_p50").desc())
)
