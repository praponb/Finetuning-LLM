from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("price").desc_nulls_first(), "id")

joined = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.stock_qty"), F.col("d.category"), F.col("d.price"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.sale_date",
        "a.revenue",
        "a.stock_qty",
        "a.category",
        F.col("b.store_name"),
    )
)

base = (
    enriched
    .filter(F.col("category").isNotNull())
    .select(
        "id",
        "store_id",
        "revenue",
        "stock_qty",
        "category",
        (F.col("stock_qty") + F.col("revenue")).alias("stock_qty_plus_revenue"),
        F.coalesce("category", F.lit("UNKNOWN")).alias("resolved_category"),
    )
)

result = (
    base
    .select("id", "store_id", "stock_qty_plus_revenue", "revenue")
    .unpivot(
        ids=["id", "store_id"],
        values=["stock_qty_plus_revenue", "revenue"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
