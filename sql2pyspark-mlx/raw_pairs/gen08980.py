from pyspark.sql import functions as F

metrics_long = (
    df_orders
    .select("id", "status", "discount", "shipping_fee", "amount")
    .unpivot(
        ids=["id", "status"],
        values=["discount", "shipping_fee", "amount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .withColumn("value_tier", F.when(F.col("value") >= 63, "HIGH").otherwise("LOW"))
    .cube("measure", "value_tier")
    .agg(
        F.max(F.when(F.col("measure") == "shipping_fee", F.col("value"))).alias("shipping_fee_max_value"),
        F.max(F.when(F.col("measure") == "discount", F.col("value"))).alias("discount_max_value"),
        F.grouping("measure").alias("grouping_id"),
    )
    .filter(F.col("shipping_fee_max_value") >= 0)
    .orderBy(F.col("shipping_fee_max_value").desc_nulls_first())
)
