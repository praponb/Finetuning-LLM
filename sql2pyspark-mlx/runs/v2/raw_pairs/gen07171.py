from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country", "city").orderBy("rating")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.count("*").over(w).alias("country_count"),
        F.percent_rank().over(w2).alias("rating_pct_rank"),
    )
    .orderBy(F.col("country").desc())
)
