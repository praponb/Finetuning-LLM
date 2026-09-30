from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("stock_qty") > 235) & (F.col("stock_qty") == 342))
    .withColumn("category_upper", F.upper("category"))
)

brand_group_summary = (
    base
    .groupBy(
        F.when(F.col("brand").isin(["Umbrella", "Initech"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
)

result = brand_group_summary.select("brand_group", "stddev_price")
