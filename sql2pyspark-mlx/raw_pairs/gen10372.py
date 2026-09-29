from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("rn").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("level") == "L2")
    .withColumn("weeks_since_hire_date", (F.datediff(F.current_date(), "hire_date") / 7))
    .withColumn("level_prefix", F.substring("level", 1, 3))
    .withColumn("level_filled", F.coalesce("level", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-03-08"))) & F.col("full_name").like("Car%"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "hire_date",
        "is_remote",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.manager_id",
        "s.hire_date",
        "s.is_remote",
        "s.job_title_clean",
        F.col("e.job_title"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select("id", "full_name", "job_title")
)

result = (
    latest
    .groupBy("job_title")
    .agg(
        F.count_if(F.col("job_title") == "Analyst").alias("analyst_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
    )
    .orderBy(F.col("analyst_events").desc_nulls_first())
)
