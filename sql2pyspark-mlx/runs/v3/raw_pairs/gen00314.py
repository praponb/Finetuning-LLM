from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "JP"]))
    .select(
        "id",
        "rating",
        "city",
        F.percent_rank().over(w).alias("rating_pct_rank"),
        F.avg("rating").over(w2).alias("city_avg_rating"),
    )
)
