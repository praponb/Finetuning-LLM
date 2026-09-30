from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Sales", "Legal"]))
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(~F.col("location").isin(["New York", "London", "Tokyo"]))
        .select("dept_name", "location")
    )
    .orderBy("location")
)
