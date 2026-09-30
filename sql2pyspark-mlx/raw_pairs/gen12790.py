from pyspark.sql import functions as F

result = (
    df_orders
    .cube(
        F.when(F.col("discount") >= 0.37, "HIGH").otherwise("LOW").alias("discount_tier"),
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        "status",
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
    .orderBy(F.col("distinct_customer_ids").desc_nulls_first())
)
