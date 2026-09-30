from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy(F.col("stock_qty_scaled").desc_nulls_last(), "id")

base = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Initech")))
    .select(
        "*",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.round((F.col("stock_qty") * 100), 2).alias("stock_qty_scaled"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
