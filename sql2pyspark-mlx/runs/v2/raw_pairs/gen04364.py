from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eur%"))
    .select(
        "id",
        "country",
        "rating",
        F.dense_rank().over(w).alias("rating_dense_rank"),
    )
)
