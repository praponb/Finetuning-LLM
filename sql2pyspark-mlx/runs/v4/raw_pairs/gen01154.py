from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

enriched = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.supplier_name"))
)

with_last = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

enriched_products = (
    with_last
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.rating"), F.col("s.country"), F.col("s.city"))
)

enriched_products_2 = (
    enriched_products
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.city").alias("supplier_city"),
        F.col("p.rating").alias("supplier_rating"),
        F.col("p.supplier_name").alias("supplier_supplier_name"),
    )
)

daily = (
    enriched_products_2
    .groupBy("category")
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.max("rating").alias("max_rating"),
    )
)

result = (
    daily
    .select(
        "category",
        "stark_count",
        "max_rating",
        F.when(F.col("category").rlike("^(Fashion|Books)$"), "EXTENDED").when(F.col("category").rlike("^(Electronics|Home)$"), "CORE").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy(F.col("stark_count").desc_nulls_last(), "max_rating")
)
