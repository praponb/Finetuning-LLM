from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy(F.col("rating_bucket").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "id",
        "country",
        "rating",
        F.substring("supplier_name", 1, 3).alias("supplier_name_prefix"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.sum("rating").over(w_running).alias("running_rating"),
        F.ntile(5).over(w2).alias("rating_bucket"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price").between(600, 1130)), "left_anti")
)

ranked = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "supplier_name_prefix")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.brand"))
)

result = (
    latest_match
    .select(
        "id",
        "supplier_name_prefix",
        "category",
        "brand",
        F.when(F.col("brand").rlike("^(Initech|Acme)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("brand_zone"),
    )
    .orderBy("brand")
)
