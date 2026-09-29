from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.concat_ws(",", "device", "country").alias("device_country_key"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country"),
    )
)

result = (
    enriched
    .select(
        "id",
        "browser",
        "country",
        "device_country_key",
        "dedup_rank",
        "seq_no",
        "seq_no_2",
        "first_known_country",
        F.when(F.col("country").rlike("^(SG|TH)$"), "PRIORITY").when(F.col("country").rlike("^(US|JP)$"), "CORE").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("dedup_rank").desc())
)
