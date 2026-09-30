from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("rating").between(3.3, 3.7))
    .select(
        "id",
        "city",
        "rating",
        F.row_number().over(w).alias("row_num"),
        F.row_number().over(w).alias("row_num_2"),
    )
)
