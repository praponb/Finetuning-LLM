from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating")
w2 = Window.orderBy(F.col("rating").desc())
w_hist = Window.partitionBy("country")

remaining_keys = (
    df_suppliers
    .select("country")
    .subtract(
        df_suppliers
        .filter(~F.col("city").isin(["Tokyo", "New York", "London"]))
        .select("country")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("rating_cume_dist", F.cume_dist().over(w))
    .withColumn("rating_rank", F.rank().over(w2))
    .withColumn("country_sum_rating", F.sum("rating").over(w_hist))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") > "2023-09-05"), "left_semi")
)

result = (
    not_flagged
    .groupBy("city")
    .pivot("country", ["TH", "JP"])
    .agg(F.avg("country_sum_rating"))
)
