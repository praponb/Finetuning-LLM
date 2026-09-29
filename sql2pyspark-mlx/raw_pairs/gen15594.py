from pyspark.sql import functions as F

result = (
    df_orders
    .rollup(
        F.when(F.col("amount") >= 1210, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.when(F.col("discount") >= 0.33, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("status").alias("distinct_status"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("amount"))).alias("returned_max_amount"),
    )
    .filter(F.col("distinct_customer_ids") >= 1)
    .orderBy("distinct_status")
)
