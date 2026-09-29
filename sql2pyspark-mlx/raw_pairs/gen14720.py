from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")
w2 = Window.partitionBy("city").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("rating", "id")
w4 = Window.partitionBy("country").orderBy("latest_rank", "id")
w5 = Window.partitionBy("country", "city").orderBy(F.col("event_seq").desc(), "id")

ranked = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .withColumn("running_rating", F.sum("rating").over(w2))
    .withColumn("prev_rating", F.lag("rating", 2).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

eligible = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") > 380), "left_semi")
)

ranked_suppliers = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "country",
        "city",
        "rating",
        "latest_rank",
        "running_rating",
        "event_seq",
    )
)

result = (
    ranked_suppliers
    .select("id", "country", "rating", "latest_rank", "running_rating", "event_seq")
    .orderBy("event_seq")
)
