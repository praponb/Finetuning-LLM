from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("shipped_date") <= "2024-10-01") & F.col("delivered_date").between("2023-03-01", "2024-02-16") & F.col("carrier").isin(["Kerry", "FedEx", "Flash"])))
    .groupBy("order_id")
    .agg(F.min("cost").alias("min_cost"))
    .orderBy(F.col("min_cost").desc())
)
