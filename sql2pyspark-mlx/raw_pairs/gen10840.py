from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price", "id")
w2 = Window.partitionBy("product_brand").orderBy("price", "id")
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("brand").orderBy("row_seq", "id")
w5 = Window.orderBy("price_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("row_seq", "id")
w7 = Window.partitionBy("brand").orderBy("row_seq_bucket", "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-08-07"))) & F.col("product_id").isin([3, 7, 9]))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 3900, "High").when(F.col("revenue") >= 1700, "Medium").otherwise("Low").alias("revenue_band"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "premium"))
    .select(
        "src.id",
        "src.product_id",
        "src.revenue_band",
        F.col("p.brand"),
        F.col("p.product_name"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.category"))
)

enriched_store_sales = (
    enriched
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.revenue_band",
        "s.brand",
        "s.product_name",
        F.col("p.price"),
        F.col("p.brand").alias("product_brand"),
    )
)

enriched_store_sales_2 = (
    enriched_store_sales
    .select(
        "id",
        "brand",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
    )
)

enriched_store_sales_3 = (
    enriched_store_sales_2
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("first_known_brand", F.first("brand", ignorenulls=True).over(w5))
    .withColumn("row_seq_bucket", F.ntile(4).over(w6))
)

latest = (
    enriched_store_sales_3
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "brand", "row_seq", "seq_no", "first_known_brand", "row_seq_bucket")
)

result = (
    latest
    .select("id", "seq_no", "row_seq_bucket")
    .orderBy(F.col("row_seq_bucket").desc())
)
