from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("launch_date").between("2023-05-13", "2024-03-01"))
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "cost",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
)
