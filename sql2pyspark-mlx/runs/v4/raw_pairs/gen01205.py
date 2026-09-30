from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy("rating_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("rating_whole", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("rating_whole", "id")
w_grp_2 = Window.partitionBy("city").orderBy("rating", "id")
w7 = Window.partitionBy("city").orderBy("rating")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("rating").between(2.3, 3.8))
    .withColumn("rating_whole", F.ceil("rating"))
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    deduped
    .select(
        "*",
        F.sum("rating").over(w2).alias("cumulative_rating"),
        F.sum("rating_whole").over(w_grp).alias("running_rating_whole"),
        F.avg("rating_whole").over(w_ordered).alias("rating_whole_avg_3"),
        F.row_number().over(w5).alias("seq_no"),
        F.lag("rating", 1).over(w_grp_2).alias("prev_rating"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"))
)

result = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "rating_whole",
        "running_rating_whole",
        "rating_whole_avg_3",
        "seq_no",
        "product_brand",
    )
    .orderBy("running_rating_whole")
)
