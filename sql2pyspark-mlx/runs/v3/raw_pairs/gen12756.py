from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w2 = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.percent_rank().over(w).alias("rating_pct_rank"),
        F.rank().over(w2).alias("rating_rank"),
    )
    .orderBy("city")
)
