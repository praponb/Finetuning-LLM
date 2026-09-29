from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "country", "rating", "supplier_name")
)
