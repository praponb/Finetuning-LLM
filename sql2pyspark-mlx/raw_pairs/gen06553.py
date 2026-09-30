from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.ntile(5).over(w).alias("rating_bucket"),
        F.count("rating").over(w2).alias("country_count_rating"),
    )
)
