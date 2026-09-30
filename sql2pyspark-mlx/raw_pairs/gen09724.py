from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("rating", "id")
w_running = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w_running_2 = Window.partitionBy("city", "country")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy("city_stddev_samp_rating", "id")
w7 = Window.partitionBy("country").orderBy(F.col("next_rating").desc(), "id")
w8 = Window.orderBy("next_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

with_windows = (
    with_last
    .filter(F.col("country").isin(["SG", "JP"]))
    .select(
        "id",
        "country",
        "rating",
        F.lead("rating", 2).over(w2).alias("next_rating"),
        F.ntile(4).over(w_running).alias("rating_bucket"),
        F.stddev_samp("rating").over(w_running_2).alias("city_stddev_samp_rating"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_running))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "country",
        "rating",
        "rating_bucket",
        "row_num",
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
        F.ntile(5).over(w6).alias("city_stddev_samp_rating_bucket"),
        F.ntile(10).over(w7).alias("next_rating_bucket"),
        F.sum("city_stddev_samp_rating").over(w8).alias("running_city_stddev_samp_rating"),
    )
)

result = (
    metrics
    .select(
        "id",
        "country",
        "rating",
        "rating_bucket",
        "row_num",
        "city_stddev_samp_rating_bucket",
        "running_city_stddev_samp_rating",
    )
    .orderBy(F.col("row_num").desc())
)
