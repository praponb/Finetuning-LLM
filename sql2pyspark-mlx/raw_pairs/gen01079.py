from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.length(F.col("ch.product_name")) >= 9), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") < 490), "left_semi")
)

scored = (
    not_flagged
    .select(
        "id",
        "supplier_name",
        "country",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
    )
)

matched_keys = (
    scored
    .select("country", "id")
    .subtract(
        df_suppliers
        .filter(F.col("country") != "JP")
        .select("country", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged_suppliers = (
    kept_rows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "bestseller")), "left_anti")
)

result = (
    not_flagged_suppliers
    .groupBy(
        F.when(F.col("seq_no") >= 7, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum("seq_no_2").alias("total_seq_no_2"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("seq_no_tier").asc_nulls_first())
)
