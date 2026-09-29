from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w_grp = Window.partitionBy("city").orderBy("rank_in_group", "id")

base = (
    df_suppliers
    .filter(F.col("country").isin(["US", "TH", "DE"]))
    .select(
        "*",
        F.concat("supplier_name", F.lit(", "), "city").alias("supplier_name_city"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand") == "Initech"), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

ranked = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.lag("rating").over(w_grp).alias("prev_rating"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "supplier_name",
        "country",
        "supplier_name_city",
        "product_launch_date",
    )
)
