from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("revenue", "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

enriched = (
    df_store_sales
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w)), 4))
    .withColumn("running_units", F.sum("units").over(w2))
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w3))
    .withColumn("next_units", F.lead("units", 2).over(w4))
)

result = (
    enriched
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "running_units",
        "first_known_units",
        "next_units",
        F.col("min_store_sale_units"),
    )
    .orderBy("revenue")
)
