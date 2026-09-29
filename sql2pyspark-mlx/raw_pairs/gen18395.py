from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.percent_rank().over(w).alias("rating_pct_rank"),
    )
)
