from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "brand").orderBy(F.col("brand_length").desc(), "id")
w2 = Window.partitionBy("brand", "country").orderBy(F.col("brand_length").desc(), "id")

base = (
    df_products
    .filter(F.col("supplier_id").isin([12, 24, 30]) & (F.col("launch_date") > F.lit("2023-03-17").cast("date")))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.lower("brand").alias("brand_lower"),
        F.substring("category", 1, 4).alias("category_prefix"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.id",
        "src.category",
        "src.brand",
        "src.cost",
        "src.launch_date",
        "src.supplier_id",
        "src.brand_lower",
        "src.resolved_brand",
        F.col("p.city"),
        F.col("p.rating"),
        F.col("p.supplier_name"),
    )
)

cleaned = (
    enriched
    .filter((F.col("launch_date") > F.lit("2023-09-07").cast("date")) & F.col("supplier_id").isNotNull())
    .select("*", F.length("brand").alias("brand_length"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") <= 3), "left_anti")
)

with_dims = (
    kept
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.brand",
        "a.launch_date",
        "a.supplier_id",
        "a.brand_lower",
        "a.city",
        "a.rating",
        "a.brand_length",
        F.col("p.country"),
        F.col("p.supplier_name").alias("supplier_supplier_name"),
    )
)

metrics = (
    with_dims
    .filter(F.col("rating").between(2.2, 4.1))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(4).over(w2).alias("brand_length_bucket"),
    )
)

kept_keys = (
    metrics
    .select("supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("supplier_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

daily = (
    kept_rows
    .cube("city")
    .agg(
        F.round(F.sum("brand_length_bucket") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("brand_length_bucket_per_acme"),
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
        F.sum(F.when(F.col("country") == "TH", F.col("seq_no")).otherwise(0)).alias("th_seq_no"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
    )
    .filter(F.col("brand_length_bucket_per_acme") > 0)
)

result = (
    daily
    .select(
        "city",
        "brand_length_bucket_per_acme",
        "th_seq_no",
        "brand_list",
        "umbrella_count",
        F.when(F.col("city").rlike("^(Berlin|Tokyo)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy("umbrella_count", "city_zone")
)
