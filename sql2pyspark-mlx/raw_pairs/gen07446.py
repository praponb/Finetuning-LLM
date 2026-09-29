from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())
w2 = Window.orderBy(F.col("rating").asc_nulls_first())
w3 = Window.partitionBy("city")

kept_keys = (
    df_suppliers
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(F.col("city") == "New York")
        .select("city", "country")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").like("%Off%")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Fashion"), "left_anti")
)

metrics = (
    kept
    .select(
        "supplier_name",
        "country",
        "rating",
        F.cume_dist().over(w).alias("rating_cume_dist"),
        F.percent_rank().over(w2).alias("rating_percent_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

filtered = (
    metrics
    .filter(F.col("country").isin(["US", "JP", "TH"]))
    .select("*", F.round((F.col("rating") * 0.9), 2).alias("adjusted_rating"))
)

result = (
    filtered
    .select("country", "rating", "rating_cume_dist", "rating_share")
    .orderBy(F.col("rating_cume_dist").asc_nulls_first(), F.col("rating_share").desc())
)
