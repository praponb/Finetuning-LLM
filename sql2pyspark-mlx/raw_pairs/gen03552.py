from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("price").asc_nulls_last(), "id")
w3 = Window.partitionBy("category", "supplier_id").orderBy(F.col("quantity").desc(), "id")

with_latest = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"), F.col("c.quantity"))
)

latest = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "category", "brand", "stock_qty", "tags", "supplier_id", "quantity")
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.tags",
        "a.supplier_id",
        "a.quantity",
        F.col("p.supplier_name"),
        F.col("p.country"),
        F.col("p.rating"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id").eqNullSafe(F.col("s.id")), "inner")
    .select(
        "src.*",
        F.col("s.rating").alias("supplier_rating"),
        F.col("s.supplier_name").alias("supplier_supplier_name"),
        F.col("s.country").alias("supplier_country"),
    )
)

result = (
    enriched
    .groupBy("supplier_id")
    .pivot("supplier_country", ["US", "TH", "UK"])
    .agg(F.max("quantity").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
