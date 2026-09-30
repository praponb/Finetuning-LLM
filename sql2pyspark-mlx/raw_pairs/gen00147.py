from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("product_launch_date", "id")
w3 = Window.partitionBy(F.year("product_launch_date"), "country").orderBy(F.col("product_launch_date").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("YEAR", "product_launch_date"), "country").orderBy(F.col("product_launch_date").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("country").isin(["JP", "US"]))
    .withColumn("supplier_name_normalized", F.upper(F.trim("supplier_name")))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isin([20, 31])), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

deduped = (
    latest_match
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

eligible_suppliers = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") < 430), "left_anti")
)

not_flagged = (
    eligible_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= F.col("s.product_launch_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "country",
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)
