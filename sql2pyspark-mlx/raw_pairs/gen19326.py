from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("rating_share", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("country").orderBy("rating_share", "id")
w_grp = Window.partitionBy("country")
w8 = Window.partitionBy("city").orderBy("cumulative_rating_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_suppliers
    .withColumn("rating_avg_5", F.avg("rating").over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
    .withColumn("rating_avg_6", F.avg("rating").over(w3))
)

scored = (
    enriched
    .withColumn("cumulative_rating_avg_5", F.sum("rating_avg_5").over(w4))
    .withColumn("rating_avg_5_avg_6_prior", F.avg("rating_avg_5").over(w5))
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .filter(F.col("city").isin(["New York", "Chiang Mai"]))
    .select(
        "*",
        F.max("cumulative_rating_avg_5").over(w_grp).alias("country_max_cumulative_rating_avg_5"),
        F.sum("cumulative_rating_avg_5").over(w8).alias("running_cumulative_rating_avg_5"),
        F.round((F.col("rating_avg_6") / F.sum("rating_avg_6").over(w2)), 4).alias("rating_avg_6_share"),
    )
)

result = (
    with_windows
    .select("id", "city", "rating_share", "rating", "rating_avg_6_share")
    .unpivot(
        ids=["id", "city"],
        values=["rating_share", "rating", "rating_avg_6_share"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
