from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("region").isNull())
    .select(
        "id",
        "region",
        "city",
        "opened_date",
        "sqft",
        F.percent_rank().over(w).alias("opened_date_pct_rank"),
        F.rank().over(w2).alias("sqft_rank"),
    )
    .orderBy("opened_date_pct_rank")
)
