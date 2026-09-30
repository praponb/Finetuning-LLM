from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy(F.col("price").desc(), "id")
w2 = Window.orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("supplier_id", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-06-07").cast("date")) & (F.col("stock_qty") == 132))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.when(F.col("stock_qty") >= 392, "High").when(F.col("stock_qty") >= 143, "Medium").otherwise("Low").alias("stock_qty_band"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.sum("cost").over(w3).alias("cumulative_cost"),
        F.sum("stock_qty").over(w4).alias("brand_sum_stock_qty"),
    )
)

enriched_products = (
    enriched
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .filter(F.col("p.rating").between(2.2, 2.3))
    .select(
        "e.id",
        "e.category",
        "e.cost",
        "e.supplier_id",
        "e.cumulative_cost",
        F.col("p.country"),
        F.col("p.city"),
    )
)

result = (
    enriched_products
    .groupBy(
        F.when(F.col("cost") >= 730, "HIGH").otherwise("LOW").alias("cost_tier"),
        "country",
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("country").alias("distinct_countries_2"),
    )
    .filter(F.col("distinct_countries_2") > 1)
)
