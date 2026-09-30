from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Bangkok"))) & F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNotNull()), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Umbrella"), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
        F.ntile(3).over(w3).alias("rating_bucket"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating",
        "s.supplier_name_slug",
        "s.row_seq",
        "s.rating_dense_rank",
        "s.rating_bucket",
        F.col("c.price"),
        F.col("c.stock_qty"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "rating",
        "supplier_name_slug",
        "row_seq",
        "rating_dense_rank",
        "price",
        "stock_qty",
    )
)
