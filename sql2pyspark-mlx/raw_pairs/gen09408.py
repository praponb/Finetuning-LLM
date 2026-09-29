from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Chiang Mai", "Berlin"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.count("rating").over(w).alias("last4_count_rating"),
        F.min("rating").over(w).alias("last4_min_rating"),
        F.avg("rating").over(w).alias("last4_avg_rating"),
    )
)
