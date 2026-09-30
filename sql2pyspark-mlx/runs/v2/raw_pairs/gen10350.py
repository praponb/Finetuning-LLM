from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.percent_rank().over(w).alias("rating_pct_rank"),
    )
)
