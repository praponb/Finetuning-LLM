from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "category").orderBy(F.col("price").desc(), "id")
w3 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w4 = Window.partitionBy("category").orderBy("launch_date", "id")

with_windows = (
    df_products
    .select(
        "*",
        F.ntile(10).over(w).alias("stock_qty_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)

remaining_keys = (
    with_windows
    .select("id")
    .subtract(
        df_products
        .filter(F.col("stock_qty").between(271, 326))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.cost",
        "a.stock_qty",
        "a.launch_date",
        "a.tags",
        "a.supplier_id",
        "a.seq_no",
        F.col("s.city"),
        F.col("s.country"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.*",
        F.col("b.supplier_name"),
        F.col("b.country").alias("supplier_country"),
        F.col("b.rating"),
    )
)

enriched_products = (
    with_dims
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .filter(F.col("d.city").isNull())
    .select("src.*", F.col("d.supplier_name").alias("supplier_supplier_name"))
)

result = (
    enriched_products
    .groupBy("country")
    .pivot("supplier_country", ["SG", "US"])
    .agg(F.max("stock_qty").alias("value_sum"))
)
