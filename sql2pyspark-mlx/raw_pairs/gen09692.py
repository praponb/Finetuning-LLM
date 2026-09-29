from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("row_num").desc(), "id")

deduped = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped_sessions = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "browser", "started_at", "country", "row_num")
)

enriched = (
    deduped_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.upper(F.col("c.name")).like("%MMA%"))
    .select("s.*", F.col("c.age"))
)

valid = (
    enriched
    .filter((F.col("started_at") > F.lit("2023-05-10").cast("date")) & (F.col("country") == "DE"))
    .withColumn("age_str", F.col("age").cast("string"))
    .withColumn("age_per_row_num", F.round((F.col("age") / F.col("row_num")), 2))
    .withColumn("country_clean", F.coalesce("country", F.lit("NONE")))
)

with_dims = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.name"), F.col("p.segment"))
)

stats = (
    with_dims
    .withColumn("age_per_row_num_tier", F.when(F.col("age_per_row_num") >= 60, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .cube("age_per_row_num_tier", "activity_month")
    .agg(
        F.round(F.avg("age_per_row_num"), 2).alias("avg_age_per_row_num"),
        F.round(F.sum("age_per_row_num") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("age_per_row_num_per_smb"),
        F.round(F.avg("age_per_row_num"), 2).alias("avg_age_per_row_num_2"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.grouping("age_per_row_num_tier").alias("gid"),
    )
)

result = (
    stats
    .select(
        "age_per_row_num_tier",
        "activity_month",
        "age_per_row_num_per_smb",
        "avg_age_per_row_num_2",
        "num_browsers",
        "country_list",
        "num_segments",
        "gid",
    )
)
