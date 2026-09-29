from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("job_title").orderBy("is_l5", "id")
w2 = Window.orderBy("is_l5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("month", "hire_date_month_end")).orderBy("next_is_l5", "id")
w4 = Window.partitionBy("job_title").orderBy(F.col("next_is_l5").desc_nulls_last())
w5 = Window.partitionBy("job_title").orderBy("hire_date_month_end", "id").rowsBetween(-6, -1)

valid = (
    df_employees
    .filter(F.col("email").like("%@gmail.com") & (F.col("hire_date") > F.to_date(F.lit("2023-09-12"))))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        F.when(F.col("level") == "L5", 1).otherwise(0).alias("is_l5"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

cleaned = (
    valid
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-08-16"))) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.when(F.col("is_l5") >= 73, "High").when(F.col("is_l5") >= 18, "Medium").otherwise("Low").alias("is_l5_tier"),
    )
)

with_windows = (
    cleaned
    .withColumn("next_is_l5", F.lead("is_l5", 2).over(w_part))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
)

base = (
    with_windows
    .filter(F.col("hire_date").between("2024-09-10", "2024-11-01"))
    .select(
        "id",
        "job_title",
        "is_l5",
        "job_title_clean",
        "is_l5_tier",
        "next_is_l5",
        "last_known_job_title",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)

enriched = (
    base
    .filter(F.col("next_is_l5").isNull())
    .withColumn("next_next_is_l5", F.lead("next_is_l5", 2, 0).over(w3))
    .withColumn("next_is_l5_percent_rank", F.percent_rank().over(w4))
    .withColumn("is_l5_avg_6_prior", F.avg("is_l5").over(w5))
)

grouped = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "hire_date_month_end"))
    .groupingSets(
        [["job_title", "quarter_start"], ["job_title"], []],
        "job_title",
        "quarter_start",
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count_if(F.col("job_title") == "Director").alias("director_events"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
        F.sum(F.when(F.col("job_title") == "Designer", F.col("is_l5")).otherwise(0)).alias("designer_is_l5"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
        F.grouping("job_title").alias("grouping_level"),
    )
    .filter(F.col("designer_events") > 1)
)

result = (
    grouped
    .select("job_title", "director_events", "cnt", "designer_events", "grouping_level")
    .orderBy(F.col("cnt").desc(), F.col("director_events").desc())
)
