from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy(F.col("brand_length").desc())
w2 = Window.orderBy(F.col("brand_length").asc_nulls_first())
w3 = Window.partitionBy("order_id", "product_id").orderBy("brand_length")

kept_keys = (
    df_order_items
    .select("product_id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("product_id", "order_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.order_id", "src.product_id", F.col("p.category"), F.col("p.brand"))
)

base = (
    enriched
    .filter(F.col("order_id").isin([28, 32]))
    .select(
        "*",
        F.length("brand").alias("brand_length"),
        F.when(F.col("brand").isin(["Umbrella", "Globex"]), "Priority").otherwise("Other").alias("brand_group"),
    )
)

scored = (
    base
    .select(
        "*",
        F.rank().over(w_part).alias("brand_length_rank"),
        F.rank().over(w2).alias("brand_length_rank_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("product_id").isNotNull())
    .select(
        "order_id",
        "product_id",
        "brand_length",
        "brand_length_rank_2",
        F.trim("brand").alias("clean_brand"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

enriched_order_items = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.amount"))
)

enriched_order_items_2 = enriched_order_items.withColumn("brand_length_cume_dist", F.cume_dist().over(w3))

result = (
    enriched_order_items_2
    .select("product_id", "amount", "brand_length_cume_dist")
    .unpivot(
        ids=["product_id"],
        values=["amount", "brand_length_cume_dist"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
)
