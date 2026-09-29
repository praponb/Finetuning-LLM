from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("stock_qty_rounded", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "launch_date")).orderBy(F.col("is_fashion").desc_nulls_first())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("category", "country").orderBy(F.col("launch_date").asc_nulls_last())

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("cost").between(420, 700))
    .withColumn("stock_qty_rounded", F.round("stock_qty", 0))
)

filtered = (
    valid
    .filter((F.col("launch_date") >= F.lit("2023-05-08").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "tags",
        "supplier_id",
        "stock_qty_rounded",
        F.when(F.col("category") == "Fashion", 1).otherwise(0).alias("is_fashion"),
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.row_number().over(w_hist).alias("row_seq"),
        F.cume_dist().over(w2).alias("is_fashion_cume_dist"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.city"))
)

enriched_products = (
    enriched
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"), F.col("d.rating"))
)

with_latest = (
    enriched_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

ranked = (
    with_latest
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.country").alias("supplier_country"),
        F.col("s.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.row_seq")).alias("max_row_seq"),
        F.grouping_id().alias("gid"),
    )
)
