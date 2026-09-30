from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("cost") < 650)
    .withColumn("price_band", F.when(F.col("price") >= 960, "High").when(F.col("price") >= 160, "Medium").otherwise("Low"))
    .withColumn("launch_date_period", F.date_trunc("WEEK", "launch_date"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("NONE")))
)

result = (
    cleaned
    .select("id", "brand", "price", "cost")
    .unpivot(
        ids=["id", "brand"],
        values=["price", "cost"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
