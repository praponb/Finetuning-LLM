from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        F.max("rating").over(w_part).alias("last4_max_rating"),
        F.count("rating").over(w_part).alias("last4_count_rating"),
        F.avg("rating").over(w_part).alias("last4_avg_rating"),
    )
)
