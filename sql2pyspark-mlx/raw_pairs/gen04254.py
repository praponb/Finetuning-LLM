from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("amount").asc_nulls_first(), "id")

valid = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("promptpay"))) & (F.col("paid_at") > F.to_date(F.lit("2023-07-23"))))
    .select(
        "*",
        F.upper("method").alias("method_upper"),
        F.trim("status").alias("clean_status"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
