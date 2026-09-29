from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "IN_TRANSIT", "RETURNED"]))
    .select(
        "id",
        "shipped_date",
        "cost",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.month("delivered_date").alias("delivered_date_month"),
    )
)
