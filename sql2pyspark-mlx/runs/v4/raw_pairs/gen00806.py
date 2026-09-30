from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc())
w2 = Window.orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-10-01")
    .select(
        "id",
        "region",
        "city",
        "sqft",
        "opened_date",
        F.percent_rank().over(w).alias("sqft_pct_rank"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
