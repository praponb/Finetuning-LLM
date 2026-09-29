from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Initech"))) & (F.col("category") == "Home"))
    .select(
        "*",
        F.add_months("launch_date", 1).alias("launch_date_plus_months"),
        F.initcap(F.trim("product_name")).alias("product_name_title"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

result = (
    filtered
    .select("id", "brand", F.col("cost").alias("COST_VAL"), "price")
    .unpivot(
        ids=["id", "brand"],
        values=["COST_VAL", "price"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
