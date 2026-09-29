from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") < 94) & (F.col("carrier") != "UPS"))
    .groupBy(
        F.quarter("delivered_date").alias("delivered_quarter"),
        F.year("shipped_date").alias("shipped_year"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
    )
)
