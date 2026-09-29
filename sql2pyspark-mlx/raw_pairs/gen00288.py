from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "London", "Tokyo"]))
    .select(
        "id",
        "city",
        "rating",
        (F.col("rating") - F.lag("rating", 1).over(w)).alias("rating_change"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy(F.col("row_num").desc())
)
