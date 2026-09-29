from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id")

ranked = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "full_name", "job_title")
)

cleaned = (
    ranked
    .filter(F.col("job_title") == "Manager")
    .withColumn("full_name_slug", F.regexp_replace("full_name", " ", "_"))
    .withColumn("job_title_prefix", F.substring("job_title", 1, 4))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("DIRECT")))
)

filtered = (
    cleaned
    .filter(F.col("job_title").isNull())
    .select(
        "id",
        "full_name",
        "job_title",
        "full_name_slug",
        "job_title_prefix",
        "job_title_filled",
        F.substring("full_name", 1, 3).alias("full_name_prefix"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

base = (
    filtered
    .filter((~F.col("job_title").eqNullSafe(F.lit("Manager"))) & (F.col("job_title") == "Director"))
    .select(
        "id",
        "full_name",
        "job_title",
        "job_title_prefix",
        "job_title_filled",
        "full_name_prefix",
        "resolved_job_title",
        F.lower("job_title").alias("job_title_lower"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title_2"),
    )
)

base_employees = (
    base
    .filter(F.col("job_title").isNotNull())
    .select("*", F.upper("job_title").alias("job_title_upper"))
)

cleaned_employees = (
    base_employees
    .filter(F.col("job_title").isNotNull() & (F.length("full_name") < 14))
    .select(
        "id",
        "full_name",
        "job_title",
        "job_title_prefix",
        "job_title_filled",
        "resolved_job_title",
        "resolved_job_title_2",
        "job_title_upper",
        F.lower("full_name").alias("full_name_lower"),
        F.length("full_name").alias("full_name_length"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_clean"),
    )
)

result = (
    cleaned_employees
    .groupBy("job_title")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count("*").alias("cnt"),
        F.percentile_approx("full_name_length", 0.5).alias("full_name_length_p50"),
    )
)
