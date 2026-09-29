from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.ntile(2).over(w).alias("quartile"),
        F.rank().over(w2).alias("rating_rank"),
    )
    .orderBy("quartile")
)
