from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.orderBy(F.col("sqft").desc())
w3 = Window.partitionBy(F.year("opened_date"), "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_stores
    .select(
        "*",
        F.percent_rank().over(w).alias("sqft_percent_rank"),
        F.percent_rank().over(w2).alias("sqft_percent_rank_2"),
        F.sum("sqft").over(w3).alias("cumulative_sqft"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_percent_rank",
        "cumulative_sqft",
    )
    .orderBy("region")
)
