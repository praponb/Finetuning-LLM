from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "rating",
        "country",
        F.sum("rating").over(w).alias("running_rating"),
        F.avg("rating").over(w2).alias("country_avg_rating"),
    )
)
