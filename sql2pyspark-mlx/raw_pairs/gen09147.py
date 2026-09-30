from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("rating", "id")
w_part = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w_recent = Window.partitionBy("country").orderBy(F.col("prev_rating").desc_nulls_first())

enriched = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.lag("rating", 2).over(w_grp).alias("prev_rating"),
        F.first("rating", ignorenulls=True).over(w_part).alias("first_known_rating"),
    )
)

kept_keys = (
    enriched
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.col("rating").between(3.0, 3.3))
        .select("country")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isin([18, 23, 29, 36])), "left_anti")
)

matched_keys = (
    not_flagged
    .select("country", "id")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").like("%Pacif%"))
        .select("country", "id")
    )
)

kept_rows_suppliers = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows_suppliers
    .select(
        "id",
        "country",
        F.round((F.col("first_known_rating") / F.sum("first_known_rating").over(w3)), 4).alias("first_known_rating_share"),
        F.rank().over(w_recent).alias("prev_rating_rank"),
    )
)
