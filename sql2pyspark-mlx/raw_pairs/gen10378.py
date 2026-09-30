from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 1.4, "Medium").otherwise("Low").alias("rating_level"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price").isNull()), "left_anti")
)
