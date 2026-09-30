from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([5, 7, 10]) & F.col("order_id").isin([19, 21, 24, 37]) & F.col("status").isin(["IN_TRANSIT", "DELIVERED"]))
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("shipped_date").alias("last_shipped_date"),
    )
    .orderBy("last_shipped_date")
)
