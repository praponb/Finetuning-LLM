from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w_part = Window.partitionBy("status", "plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("monthly_fee").desc())
w4 = Window.partitionBy("status", "plan").orderBy("start_date", "id")
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())
w6 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_rank").desc(), "id")
w7 = Window.partitionBy("plan").orderBy(F.col("prev_monthly_fee").desc_nulls_last(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-11-03"))) & F.col("customer_id").isNotNull())
    .select("*", F.concat("status", F.lit(" - "), "plan").alias("status_plan"))
)

enriched = (
    cleaned
    .select(
        "id",
        "plan",
        "end_date",
        F.percent_rank().over(w).alias("monthly_fee_percent_rank"),
        F.sum("monthly_fee").over(w_part).alias("running_monthly_fee"),
        F.rank().over(w3).alias("monthly_fee_rank"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee"),
        F.percent_rank().over(w5).alias("monthly_fee_percent_rank_2"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee_percent_rank",
        "monthly_fee_rank",
        "prev_monthly_fee",
        "dedup_rank",
        "rn",
        F.when(F.col("plan").rlike("^(premium|enterprise)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(basic|standard)$"), "EXTENDED").otherwise("CORE").alias("plan_zone"),
    )
)
