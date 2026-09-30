from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").desc_nulls_first(), "id")

latest = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "method", "amount", "status")
)

result = (
    latest
    .groupBy("method")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_rows"),
    )
    .orderBy("method")
)
