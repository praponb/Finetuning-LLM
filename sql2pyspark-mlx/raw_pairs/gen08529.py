from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w2 = Window.orderBy("stock_qty", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("category", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("stock_qty", "id")
w5 = Window.partitionBy("store_id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("units").alias("avg_store_sale_units"))

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-09-02").cast("date")) & F.col("product_id").isin([2, 9, 10]))
    .select("*", F.dayofmonth("last_restock").alias("last_restock_day"))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "product_id", "quantity", "last_restock_day")
)

enriched = (
    latest
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.category"), F.col("b.stock_qty"))
)

scored = (
    enriched
    .select(
        "*",
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_3_prior"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.lag("stock_qty", 1).over(w4).alias("prev_stock_qty"),
        F.avg("last_restock_day").over(w5).alias("store_id_avg_last_restock_day"),
    )
)

result = (
    scored
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock_day",
        "category",
        "last_known_quantity",
        "prev_stock_qty",
        "store_id_avg_last_restock_day",
        F.col("avg_store_sale_units"),
    )
)
