from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.orderBy(F.col("rating").desc(), "id")
w3 = Window.orderBy(F.col("adjusted_rating_bucket").desc(), "id")
w4 = Window.orderBy("rating_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") < 320), "left_semi")
)

with_windows = (
    kept
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("rating_bucket", F.ntile(10).over(w2))
)

valid = (
    with_windows
    .filter(F.col("rating") > 2.8)
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_bucket",
        F.round((F.col("rating_bucket") * 0.85), 2).alias("adjusted_rating_bucket"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") > 230), "left_semi")
)

with_windows_suppliers = (
    eligible
    .select(
        "*",
        F.ntile(5).over(w3).alias("adjusted_rating_bucket_bucket"),
        F.last("adjusted_rating_bucket", ignorenulls=True).over(w4).alias("last_known_adjusted_rating_bucket"),
    )
)

remaining_keys = (
    with_windows_suppliers
    .select("id")
    .intersectAll(
        df_suppliers
        .filter(~F.col("country").isin(["UK", "US"]))
        .select("id")
    )
)

kept_rows = (
    with_windows_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_suppliers = (
    kept_rows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand").isin(["Initech", "Stark", "Acme"])), "left_semi")
)

report = (
    kept_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_bucket",
        "adjusted_rating_bucket",
        "adjusted_rating_bucket_bucket",
        "last_known_adjusted_rating_bucket",
    )
    .orderBy(
        F.col("adjusted_rating_bucket").asc_nulls_last(),
        "adjusted_rating_bucket_bucket",
    )
)

result = (
    report
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_bucket",
        "adjusted_rating_bucket",
        "last_known_adjusted_rating_bucket",
        F.when(F.col("supplier_name").rlike("^(Lion City Imports|Pacific Wholesale)$"), "GROUP_A").otherwise("GROUP_B").alias("supplier_name_zone"),
    )
)
