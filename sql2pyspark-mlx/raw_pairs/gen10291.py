from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("sale_date").desc())
w2 = Window.partitionBy("category").orderBy(F.col("units").desc(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

joined = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.product_name"), F.col("d.brand"), F.col("d.category"))
)

valid = (
    joined
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-05-16")))
    .select("*", F.lower("category").alias("category_lower"))
)

with_dims = (
    valid
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"))
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "product_name",
        "brand",
        "category",
        "category_lower",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
