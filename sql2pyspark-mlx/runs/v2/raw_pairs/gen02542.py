from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") <= "2023-03-01") & F.col("order_id").isin([10, 13, 24, 28]) & F.col("carrier").isin(["Flash", "Kerry", "FedEx"]))
    .groupBy(
        F.when(F.col("cost") >= 107, "high").when(F.col("cost") >= 20, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.max("cost").alias("max_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") > 27)
    .drop("min_cost")
    .orderBy("unique_statuss")
)
